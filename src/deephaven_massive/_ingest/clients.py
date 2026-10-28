"""`SessionClients`, the one holder of a session's wire clients and connections.

A session talks to Massive through the wire package's clients (`_massive.RestClient` for the REST
API, one `_massive.WsClient` per WebSocket connection, owned by the session's `WsConnections`,
and `_massive.FlatFileClient` for the S3 flat files). Each client is per session: it carries the
session's credentials and connection pool and is closed with the session. `SessionClients` owns
them all, so nothing on the Deephaven side (`_tables/`) ever holds a wire client, and the session
core reaches the transports only through
the `IngestPorts` protocol it defines (`rest()`, `streams()`, `flat_files()`).

`MassiveSession.__init__` creates the core, then `SessionClients(core)`, then attaches the
clients to the core once (`SessionCore.attach`). Each client is created on first use and its
close is registered with the core (`SessionCore.on_close`), so it closes with the session, or
on the reaper thread if the session is collected without `close()`.

One thing is not per session: the REST `RequestGovernor`, which every session of the process
with the same API key and REST origin shares, as Massive counts their requests together.
`SessionClients` takes its lease when it is created (`_ingest/rate_limits.py`), feeds the
governor's state to `m.rate_limits()`, and releases the lease with the session.
"""

from __future__ import annotations

import functools
import logging
import weakref
from typing import TYPE_CHECKING

from deephaven_massive import _massive
from deephaven_massive._errors import MassiveError
from deephaven_massive._ingest import rate_limits
from deephaven_massive._ingest.flatfiles import FlatFileHistory
from deephaven_massive._ingest.rest import RestEndpoints, RestState
from deephaven_massive._ingest.ws import WsConnections, WsTransport

if TYPE_CHECKING:
    from deephaven_massive._datasets.request import HostDecision
    from deephaven_massive._enums import AssetClass, ChannelFamily
    from deephaven_massive._massive import FlatFileClient
    from deephaven_massive._tables.core import (
        FlatFileTransport,
        RestTransport,
        SessionCore,
        StreamTransport,
    )

__all__ = ["SessionClients"]

logger = logging.getLogger(__name__)


class SessionClients:
    """The wire clients and connections of one session, and the transports built on them.

    Created once per session by `MassiveSession.__init__`, right after the session's core, and
    attached to that core, whose transport hooks call `rest()`, `streams()`, and
    `flat_files()` (the `IngestPorts` protocol of `_tables/core.py`). The core holds this
    object, so this object holds the core only through a weak reference: a strong reference
    back would make a cycle, and a session dropped without `close()` would then wait for the
    cyclic garbage collector before its connections are closed. Each method takes a strong
    reference for the length of its call (`_live_core`) and hands the transports it builds
    the core itself, never the weak reference. Thread-safe: each client is created under the
    core's `registry_lock`, and the clients themselves are thread-safe.
    """

    __slots__ = ("_core", "_flat_files", "_label", "_lease", "_rest", "_streams")

    def __init__(self, core: SessionCore) -> None:
        """Bind the clients to their session and take the session's lease on its REST governor.

        No client is created yet. The lease comes from the process's registry
        (`rate_limits.GOVERNORS`), keyed by the configuration's API key and REST base URL,
        declaring its `rest_basic_subscriptions`; its release is registered as a session
        closer. The governor's listener and the source of the `m.rate_limits()` rows are the
        core's diagnostics, which hold no reference to the core.

        Args:
            core: The session's core, for its configuration, its `registry_lock`, its
                closers, and its diagnostics.
        """
        self._core = weakref.ref(core)
        self._label = core.label
        config = core.config
        diagnostics = core.diagnostics
        self._lease = rate_limits.GOVERNORS.lease(
            api_key=config.api_key,
            base_url=config.rest_base_url,
            basic=config.rest_basic_subscriptions,
            label=core.label,
            listener=diagnostics.rate_limits_changed,
        )
        core.on_close(self._lease.release)
        diagnostics.watch_rate_limits(
            functools.partial(rate_limits.rate_limit_source, self._lease.governor)
        )
        # Created by the first flat-file read (`flat_files`), under the core's `registry_lock`.
        self._flat_files: FlatFileClient | None = None
        # Created by the first stream (`streams`), under the core's `registry_lock`.
        self._streams: WsConnections | None = None
        # Created by the first REST call (`rest`): the client, host decisions, and pollers.
        self._rest: RestState | None = None

    def _live_core(self) -> SessionCore:
        """Return the session's core, which a caller of these methods normally keeps alive.

        Returns:
            The core.

        Raises:
            MassiveError: If the core has been garbage-collected: the session is gone, as if
                closed (`"MassiveSession #1 is closed"`).
        """
        core = self._core()
        if core is None:
            raise MassiveError(f"{self._label} is closed")
        return core

    def rest(self) -> RestTransport:
        """Return a REST transport bound to the session's core and its shared REST state.

        Each call gets a new, lightweight `RestEndpoints`. The first call creates the
        session's `RestState`: it creates the `_massive.RestClient` outside every lock, on the
        session's shared governor and with the session's `RateLimitWarnings` as its refusal
        hook (the constructor reads the proxy and certificate settings, but makes no
        request), installs it under the core's `registry_lock` unless a racing call installed
        one first (the loser's client is closed at once), and registers the state's close with
        `SessionCore.on_close` after releasing the lock. So the client closes, and every poller
        stops, with the session, or at once if the session closed meanwhile. The closer refers
        to the state only, never to the core.

        Returns:
            The transport.

        An API key or `rest_base_url` the REST client cannot use raises its `ValueError` (see
        `RestClient`), and a garbage-collected core the `MassiveError` of `_live_core`.
        """
        core = self._live_core()
        with core.registry_lock:
            state = self._rest
        if state is None:
            config = core.config
            warnings = rate_limits.RateLimitWarnings(core.label, config.rest_basic_subscriptions)
            candidate = RestState(
                _massive.RestClient(
                    config.api_key,
                    base_url=config.rest_base_url,
                    max_concurrency=config.max_concurrency,
                    governor=self._lease.governor,
                    on_refused=warnings.refused,
                ),
                core.label,
            )
            with core.registry_lock:
                state = self._rest
                created = state is None
                if state is None:
                    state = self._rest = candidate
            if created:
                core.on_close(state.close)
            else:
                candidate.close()
        return RestEndpoints(core, state)

    def streams(self) -> StreamTransport:
        """Return a WebSocket transport over the session's connections.

        Each call gets a new, lightweight `WsTransport` bound to the session's core and to the
        session's one `WsConnections`. The first call creates the `WsConnections` under the
        core's `registry_lock` (its constructor only stores settings: no connection is opened
        until a stream needs one) and registers its `close` with `SessionCore.on_close` after
        releasing the lock, so the connections close with the session, or at once if the
        session closed meanwhile. The `WsConnections` holds the core weakly, since it lives as
        long as the session, and decides each class's host through this object's `rest()`
        transport (`RestTransport.resolve_host`), reached when the decision is made.

        Returns:
            The transport.

        A garbage-collected core raises the `MassiveError` of `_live_core`.
        """
        core = self._live_core()
        with core.registry_lock:
            connections = self._streams
            created = connections is None
            if connections is None:
                connections = self._streams = WsConnections(
                    core, resolve_host=self._resolve_host, record_verdict=self._record_verdict
                )
        if created:
            core.on_close(connections.close)
        return WsTransport(core, connections)

    def _resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family) through the session's REST transport.

        Args:
            asset_class: The class.
            family: The family.

        Returns:
            The decision, cached for the session by the REST transport.
        """
        return self.rest().resolve_host(asset_class, family)

    def _record_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        """Record the server's real-time verdict as the session's host decision of a pair.

        The REST transport keeps the one cache of host decisions and the pair's REST row of
        `m.connections()`; this replaces both, so later streams of the pair use the delayed host.

        Args:
            asset_class: The class.
            family: The family.
            decision: The decision the verdict implies (`DELAYED`, evidence `server verdict: ...`).
        """
        self.rest().record_verdict(asset_class, family, decision)

    def flat_files(self) -> FlatFileTransport:
        """Return a flat-file transport for one `history()` call.

        Each call gets a new, lightweight `FlatFileHistory` bound to the session's core and to
        the session's `FlatFileClient`. The first call creates that client: it reads
        `_massive.FlatFileClient` outside every lock (the first read imports boto3, so a session
        that reads no flat file never imports it), creates the client under the core's
        `registry_lock` (the constructor only stores settings), and registers its close with
        `SessionCore.on_close` after releasing the lock, so the client closes with the session,
        or at once if the session closed meanwhile. No call makes a network request: the boto3
        client is created by the first read.

        Returns:
            The transport.

        Raises:
            ValueError: If the configuration has no S3 credentials. `history()` never gets
                here then: its source choice has already required them.

        A garbage-collected core raises the `MassiveError` of `_live_core`.
        """
        core = self._live_core()
        config = core.config
        access_key_id, secret_access_key = config.s3_access_key_id, config.s3_secret_access_key
        if access_key_id is None or secret_access_key is None:
            raise ValueError(
                "flat files need MassiveConfig s3_access_key_id and s3_secret_access_key"
            )
        factory = _massive.FlatFileClient
        with core.registry_lock:
            client = self._flat_files
            created = client is None
            if client is None:
                # The client holds no reference to the core, so its closer may outlive it.
                client = self._flat_files = factory(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    endpoint_url=config.s3_endpoint_url,
                    bucket=config.s3_bucket,
                    max_pool=config.max_concurrency,
                )
        if created:
            core.on_close(client.close)
        return FlatFileHistory(core, client)
