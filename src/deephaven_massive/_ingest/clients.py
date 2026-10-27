"""`SessionClients`, the one holder of a session's wire clients and connections.

A session talks to Massive through the wire package's clients (`_massive.FlatFileClient` for
the S3 flat files; the REST client and the WebSocket connections are not implemented yet). Each
client is per session: it carries the session's credentials and connection pool and is closed
with the session. `SessionClients` owns them all, so nothing on the Deephaven side
(`_tables/`) ever holds a wire client, and the session core reaches the transports only through
the `IngestPorts` protocol it defines (`rest()`, `streams()`, `flat_files()`).

`MassiveSession.__init__` creates the core, then `SessionClients(core)`, then attaches the
clients to the core once (`SessionCore.attach`). Each client is created on first use and its
close is registered with the core (`SessionCore.on_close`), so it closes with the session, or
on the reaper thread if the session is collected without `close()`.
"""

from __future__ import annotations

import logging
import weakref
from typing import TYPE_CHECKING

from deephaven_massive import _massive
from deephaven_massive._errors import MassiveError
from deephaven_massive._ingest.flatfiles import FlatFileHistory

if TYPE_CHECKING:
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

    __slots__ = ("_core", "_flat_files", "_label")

    def __init__(self, core: SessionCore) -> None:
        """Bind the clients to their session; no client is created yet.

        Args:
            core: The session's core, for its configuration, its `registry_lock`, and its
                closers.
        """
        self._core = weakref.ref(core)
        self._label = core.label
        # Created by the first flat-file read (`flat_files`), under the core's `registry_lock`.
        self._flat_files: FlatFileClient | None = None

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
        """Return the session's REST transport.

        Raises:
            NotImplementedError: Always, until the REST transport is implemented.
        """
        raise NotImplementedError("the REST transport is not implemented yet")

    def streams(self) -> StreamTransport:
        """Return the session's WebSocket transport.

        Raises:
            NotImplementedError: Always, until the WebSocket transport is implemented.
        """
        raise NotImplementedError("the WebSocket transport is not implemented yet")

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
