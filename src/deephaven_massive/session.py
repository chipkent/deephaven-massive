"""Top-level `MassiveSession` orchestrating Deephaven, REST, WebSockets, and 9 domain clients.

`MassiveSession` is the single entry point for interacting with Massive.com in
Deephaven. It manages the host Deephaven `ExecutionContext`, a thread-safe `urllib3`
HTTP connection pool with exponential backoff, a multiplexed WebSocket connection pool,
and nine specialized domain sub-clients (`stocks`, `options`, `futures`, `indices`,
`forex`, `crypto`, `economy`, `partners`, and `flat_files`).
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, BinaryIO
from urllib.parse import urlencode, urlsplit

import pyarrow as pa
import urllib3

from deephaven_massive.config import MassiveConfig, MassiveEntitlementError
from deephaven_massive.domains.crypto import CryptoClient
from deephaven_massive.domains.economy import EconomyClient
from deephaven_massive.domains.flat_files import FlatFilesClient
from deephaven_massive.domains.forex import ForexClient
from deephaven_massive.domains.futures import FuturesClient
from deephaven_massive.domains.indices import IndicesClient
from deephaven_massive.domains.options import OptionsClient
from deephaven_massive.domains.partners import PartnersClient
from deephaven_massive.domains.stocks import StocksClient
from deephaven_massive.engine.arrow_bridge import arrow_to_deephaven_table
from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive.engine.builder import FeedBuilder
    from deephaven_massive.engine.publisher import MicroBatchPublisher


def _ensure_deephaven_server(
    *,
    auto_start: bool = True,
    port: int = 10000,
    jvm_args: list[str] | None = None,
) -> ExecutionContext | None:
    """Ensure the embedded Deephaven server is initialized and return the host `ExecutionContext`.

    When running inside an already-started Deephaven IDE or worker, `Server.instance`
    is already populated and its active `ExecutionContext` is captured immediately.
    When running in a standalone Python script or notebook, a new embedded server is
    started on `port` if `auto_start=True`.

    Args:
        auto_start: Whether to start a new embedded `deephaven_server.Server` when
            `Server.instance` is `None`.
        port: TCP port for the embedded Deephaven server when auto-starting.
        jvm_args: Optional JVM flags passed to `Server` on startup (defaults to
            `["-Xmx4g"]`).

    Returns:
        The captured Deephaven `ExecutionContext`, or `None` if no server is running
        and `auto_start=False`.
    """
    from deephaven_server import Server

    if Server.instance is None:
        if not auto_start:
            return None
        srv = Server(port=port, jvm_args=jvm_args or ["-Xmx4g"])
        srv.start()

    from deephaven.execution_context import get_exec_ctx

    return get_exec_ctx()


class MassiveSession:
    """Primary user-facing entry point for `deephaven-massive`.

    `MassiveSession` coordinates authentication, connection pooling, background
    threads, and Deephaven table creation across all nine Massive.com data domains.
    It can be used directly in interactive Deephaven sessions or as a context manager
    (`with MassiveSession() as m:`) for deterministic cleanup.

    Attributes:
        config: Validated `MassiveConfig` instance for this session.
        exec_ctx: Captured Deephaven `ExecutionContext` used by background threads.
        ws_multiplexer: Shared `WebSocketMultiplexer` managing per-endpoint connections.
        stocks: `StocksClient` for US equities trades, quotes, bars, snapshots,
            reference data, fundamentals, corporate actions, and technical indicators.
        options: `OptionsClient` for OPRA options contracts, chains, trades, quotes,
            bars, snapshots, and technical indicators.
        futures: `FuturesClient` for CME, CBOT, NYMEX, and COMEX futures contracts,
            schedules, trades, quotes, and bars.
        indices: `IndicesClient` for market index values, bars, snapshots, and
            technical indicators.
        forex: `ForexClient` for global FX quotes, bars, snapshots, currency
            conversion, and technical indicators.
        crypto: `CryptoClient` for global crypto trades, quotes, FMV, bars,
            snapshots, and technical indicators.
        economy: `EconomyClient` for US Treasury yields, inflation, inflation
            expectations, labor market indicators, and funding conditions.
        partners: `PartnersClient` for Benzinga news/ratings/earnings/guidance/transcripts,
            ETF Global, TMX corporate events, and Fable Data.
        flat_files: `FlatFilesClient` for direct S3 `.csv.gz` flat-file streaming
            with PyArrow predicate pushdown.

    Example:
        ```python
        from deephaven_massive import MassiveSession

        m = MassiveSession(feed="realtime")
        quotes = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).latest()
        status_tbl = m.status()
        ```
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        feed: str = "realtime",
        rest_base_url: str = "https://api.massive.com",
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_endpoint_url: str = "https://files.massive.com",
        s3_bucket: str = "flatfiles",
        flush_interval_ms: int = 50,
        max_batch_size: int = 10_000,
        max_retries: int = 3,
        flat_file_ticker_threshold: int = 50,
        auto_start_server: bool = True,
        server_port: int = 10000,
        auto_flush: bool = True,
        http_handler: Callable[
            [str, Mapping[str, Any] | None, Mapping[str, str]],
            tuple[int, Any],
        ]
        | None = None,
        ws_send_hook: Callable[[str, dict[str, Any]], None] | None = None,
        ws_connector: Callable[[str], Any] | None = None,
        s3_opener: Callable[[str, str], BinaryIO] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize a `MassiveSession` and its nine domain sub-clients.

        Args:
            api_key: Massive API key. Defaults to `MASSIVE_API_KEY` or `POLYGON_API_KEY`
                environment variables.
            feed: Default WebSocket feed tier (`"realtime"`, `"delayed"`, `"business"`, etc.).
            rest_base_url: Base URL for Massive REST API requests.
            s3_access_key_id: S3 access key ID for Flat Files. Defaults to
                `MASSIVE_S3_ACCESS_KEY_ID`.
            s3_secret_access_key: S3 secret access key for Flat Files. Defaults to
                `MASSIVE_S3_SECRET_ACCESS_KEY`.
            s3_endpoint_url: S3 endpoint URL for Flat Files.
            s3_bucket: S3 bucket name for Flat Files.
            flush_interval_ms: Interval in milliseconds between micro-batch flushes.
            max_batch_size: Maximum buffered rows before immediate flush.
            max_retries: Maximum HTTP retry attempts on transient status codes.
            flat_file_ticker_threshold: Symbol count threshold at which `source="auto"`
                historical queries switch from REST pagination to S3 Flat Files.
            auto_start_server: Start an embedded Deephaven server if none is active.
            server_port: Port for the embedded Deephaven server if auto-started.
            auto_flush: Automatically start background flush threads on publishers.
            http_handler: Optional custom HTTP transport hook for deterministic testing.
            ws_send_hook: Optional WebSocket outbound control frame hook for testing.
            ws_connector: Optional custom async WebSocket connector callable.
            s3_opener: Optional custom `(bucket, key) -> BinaryIO` opener for S3 testing.
            sleep_fn: Sleep function used during HTTP retry backoff.

        Raises:
            ValueError: If API credentials or numeric configuration parameters are invalid.
        """
        self.config = MassiveConfig.from_args(
            api_key=api_key,
            feed=feed,
            rest_base_url=rest_base_url,
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_endpoint_url=s3_endpoint_url,
            s3_bucket=s3_bucket,
            flush_interval_ms=flush_interval_ms,
            max_batch_size=max_batch_size,
            max_retries=max_retries,
            flat_file_ticker_threshold=flat_file_ticker_threshold,
            auto_start_server=auto_start_server,
        )
        self._auto_flush = auto_flush
        self._http_handler = http_handler
        self._sleep_fn = sleep_fn
        self._http_pool = urllib3.PoolManager()
        self._lock = threading.RLock()

        self.exec_ctx = _ensure_deephaven_server(
            auto_start=self.config.auto_start_server,
            port=server_port,
        )
        self.ws_multiplexer = WebSocketMultiplexer(
            self.config,
            send_hook=ws_send_hook,
            ws_connector=ws_connector,
        )
        self._active_builders: list[FeedBuilder] = []
        self._stream_registry: dict[tuple[str, str, tuple[str, ...]], MicroBatchPublisher] = {}

        self.stocks = StocksClient(self)
        self.options = OptionsClient(self)
        self.futures = FuturesClient(self)
        self.indices = IndicesClient(self)
        self.forex = ForexClient(self)
        self.crypto = CryptoClient(self)
        self.economy = EconomyClient(self)
        self.partners = PartnersClient(self)
        self.flat_files = FlatFilesClient(self, s3_opener=s3_opener)

    def _register_builder(self, builder: FeedBuilder) -> None:
        """Track an active `FeedBuilder` so its background resources are cleaned up on `close`.

        Args:
            builder: Active `FeedBuilder` instance to register with this session.
        """
        with self._lock:
            if builder not in self._active_builders:
                self._active_builders.append(builder)

    def stop_stream(
        self,
        target: FeedBuilder | str | None = None,
        *,
        ws_channel: str | None = None,
        symbols: str | Iterable[str] | None = None,
    ) -> int:
        """Stop and unregister active streaming or polling `FeedBuilder` instances matching criteria.

        If called with no arguments, stops all active builders registered on this session.

        Args:
            target: Optional `FeedBuilder` instance, feed name (e.g., `"stocks_quotes"` or
                `"stocks.quotes"`), WebSocket channel (e.g., `"Q"`), or WebSocket URL to match.
            ws_channel: Optional WebSocket event prefix filter (e.g., `"T"`, `"Q"`, `"AM"`).
            symbols: Optional symbol or iterable of symbols; stops builders whose
                subscription set intersects with `symbols`.

        Returns:
            The number of active `FeedBuilder` instances that were stopped.
        """
        with self._lock:
            to_stop: list[FeedBuilder] = []
            for builder in list(self._active_builders):
                if target is not None:
                    if (
                        builder is target
                        or builder.name == target
                        or (isinstance(target, str) and builder.name == target.replace(".", "_"))
                        or builder.ws_channel == target
                        or builder.ws_url == target
                    ):
                        to_stop.append(builder)
                    continue
                if ws_channel is not None and builder.ws_channel != ws_channel:
                    continue
                if symbols is not None:
                    target_syms = set(builder._normalize_symbols(symbols))
                    if not (set(builder._canonical_symbol_list()) & target_syms):
                        continue
                to_stop.append(builder)

            for builder in to_stop:
                builder.stop()
                self._active_builders.remove(builder)
            return len(to_stop)

    def status(self) -> Table:
        """Return a Deephaven `Table` summarizing WebSocket connections and active topic counts.

        Columns returned:
        - `WsUrl` (`string`): WebSocket endpoint URL.
        - `Authenticated` (`bool`): Whether authentication succeeded on the socket.
        - `Connecting` (`bool`): Whether a background connection task is active.
        - `ActiveTopicCount` (`int64`): Count of reference-counted topics (`ref_count > 0`).
        - `ActiveTopics` (`string`): Comma-separated list of active subscription topics.
        - `LastError` (`string`): Most recent connection or entitlement error, if any.

        Returns:
            A static diagnostic Deephaven `Table`.
        """
        schema = pa.schema(
            [
                pa.field("WsUrl", pa.string()),
                pa.field("Authenticated", pa.bool_()),
                pa.field("Connecting", pa.bool_()),
                pa.field("ActiveTopicCount", pa.int64()),
                pa.field("ActiveTopics", pa.string()),
                pa.field("LastError", pa.string()),
            ]
        )
        rows = self.ws_multiplexer.endpoint_snapshots()
        pa_tbl = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
        return arrow_to_deephaven_table(pa_tbl, self.exec_ctx)

    def stats(self) -> Table:
        """Return a Deephaven `Table` summarizing active `FeedBuilder` instances and published rows.

        Columns returned:
        - `FeedName` (`string`): Logical feed name (e.g., `"stocks_trades"`).
        - `Mode` (`string`): Execution mode (`"stream"`, `"watchlist"`, or `"poll"`).
        - `WsChannel` (`string`): WebSocket channel prefix, or `None` for REST pollers.
        - `Symbols` (`string`): Comma-separated list of active canonical symbols.
        - `RowsPublished` (`int64`): Total rows published into Deephaven by the feed.

        Returns:
            A static diagnostic Deephaven `Table`.
        """
        schema = pa.schema(
            [
                pa.field("FeedName", pa.string()),
                pa.field("Mode", pa.string()),
                pa.field("WsChannel", pa.string()),
                pa.field("Symbols", pa.string()),
                pa.field("RowsPublished", pa.int64()),
            ]
        )
        with self._lock:
            builders = list(self._active_builders)
        rows: list[dict[str, Any]] = []
        for b in builders:
            if b.watchlist_controller is not None:
                mode = "watchlist"
            elif b.poller is not None:
                mode = "poll"
            else:
                mode = "stream"
            if b.publisher is not None:
                rows_pub = b.publisher.rows_published
            elif b.poller is not None:
                rows_pub = b.poller.rows_published
            else:
                rows_pub = 0
            rows.append(
                {
                    "FeedName": b.name,
                    "Mode": mode,
                    "WsChannel": b.ws_channel,
                    "Symbols": ",".join(b._canonical_symbol_list()),
                    "RowsPublished": rows_pub,
                }
            )
        pa_tbl = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
        return arrow_to_deephaven_table(pa_tbl, self.exec_ctx)

    def _build_url(self, path_or_url: str, params: Mapping[str, Any] | None) -> str:
        """Resolve a relative REST path or full URL and append non-null query parameters."""
        if path_or_url.startswith(("http://", "https://")):
            parsed_url = urlsplit(path_or_url)
            base_url = urlsplit(self.config.rest_base_url)
            if (parsed_url.scheme, parsed_url.netloc) != (base_url.scheme, base_url.netloc):
                raise ValueError(
                    f"Refusing to send authenticated Massive request to untrusted origin "
                    f"'{parsed_url.scheme}://{parsed_url.netloc}' (expected "
                    f"'{base_url.scheme}://{base_url.netloc}')"
                )
            url = path_or_url
        else:
            clean_path = path_or_url if path_or_url.startswith("/") else f"/{path_or_url}"
            url = f"{self.config.rest_base_url}{clean_path}"

        if params:
            clean_params = {k: v for k, v in params.items() if v is not None}
            if clean_params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urlencode(clean_params)}"
        return url

    def _execute_http(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, str],
    ) -> tuple[int, Any]:
        """Execute an HTTP GET request via the injected handler or the `urllib3` pool."""
        if self._http_handler is not None:
            return self._http_handler(url, params, headers)

        resp = self._http_pool.request(
            "GET",
            url,
            headers=dict(headers),
            timeout=urllib3.Timeout(connect=10.0, read=30.0),
        )
        raw_text = resp.data.decode("utf-8", errors="replace") if resp.data else "{}"
        if not raw_text.strip():
            return int(resp.status), {}
        try:
            parsed: Any = json.loads(raw_text)
        except ValueError:
            parsed = raw_text
        return int(resp.status), parsed

    def _request_json(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """Perform an authenticated GET request to a Massive REST endpoint with retry and backoff.

        Args:
            path_or_url: Relative REST path (e.g., `"/v3/reference/tickers"`) or a full
                pagination `next_url` returned by a previous response.
            params: Optional dictionary of query parameters (`None` values are omitted).

        Returns:
            Decoded JSON payload (typically a `dict` containing `"results"` or `"tickers"`).

        Raises:
            MassiveEntitlementError: If the endpoint returns HTTP 403 Forbidden.
            RuntimeError: If the request fails with a non-retryable HTTP status or
                exhausts `config.max_retries` on transient errors (`429`, `500`, `502`,
                `503`, `504`).
        """
        url = self._build_url(path_or_url, params)
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Accept": "application/json",
        }

        attempt = 0
        while True:
            status, payload = self._execute_http(url, params, headers)
            if 200 <= status < 300:
                if isinstance(payload, str):
                    raise RuntimeError(
                        f"Massive REST request to '{url}' returned invalid JSON: {payload}"
                    )
                return payload

            if status == 403:
                msg = (
                    payload.get("message") or payload.get("error")
                    if isinstance(payload, Mapping)
                    else str(payload)
                )
                raise MassiveEntitlementError(
                    f"Massive API key is not entitled to access '{url}': {msg}"
                )

            if status in (429, 500, 502, 503, 504) and attempt < self.config.max_retries:
                backoff_s = 0.5 * (2**attempt)
                self._sleep_fn(backoff_s)
                attempt += 1
                continue

            raise RuntimeError(
                f"Massive REST request to '{url}' failed with HTTP {status}: {payload}"
            )

    def close(self) -> None:
        """Stop all active streaming publishers, pollers, watchlist controllers, and WebSocket connections."""
        with self._lock:
            builders = list(self._active_builders)
            self._active_builders.clear()
            self._stream_registry.clear()
        try:
            for builder in builders:
                builder.stop()
        finally:
            self.ws_multiplexer.close()
            self._http_pool.clear()

    def __enter__(self) -> MassiveSession:
        """Enter the runtime context related to this `MassiveSession`."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the runtime context and close all active streams, pollers, and sockets."""
        self.close()
