"""Top-level MassiveSession orchestrating Deephaven ExecutionContext, REST HTTP pool, WebSocket multiplexer, and 9 domain sub-clients."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import time
from typing import TYPE_CHECKING, Any, BinaryIO
from urllib.parse import urlencode

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
from deephaven_massive.engine.ws_multiplexer import WebSocketMultiplexer

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven_massive.engine.builder import FeedBuilder


def ensure_deephaven_server(
    *,
    auto_start: bool = True,
    port: int = 10000,
    jvm_args: list[str] | None = None,
) -> ExecutionContext | None:
    """Ensure the embedded Deephaven server is initialized and return the host :class:`ExecutionContext`."""
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

    Exposes 9 domain sub-clients:
    - :attr:`stocks`: US Equities (`m.stocks`)
    - :attr:`options`: US Options OPRA (`m.options`)
    - :attr:`futures`: US Futures CME/CBOT/NYMEX/COMEX (`m.futures`)
    - :attr:`indices`: Market Indices (`m.indices`)
    - :attr:`forex`: Global Forex (`m.forex`)
    - :attr:`crypto`: Global Crypto (`m.crypto`)
    - :attr:`economy`: Federal Reserve & Macroeconomics (`m.economy`)
    - :attr:`partners`: Benzinga, ETF Global, and TMX (`m.partners`)
    - :attr:`flat_files`: Direct S3 `.csv.gz` streaming (`m.flat_files`)
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
        s3_opener: Callable[[str, str], BinaryIO] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
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
        self.auto_flush = auto_flush
        self._http_handler = http_handler
        self._sleep_fn = sleep_fn
        self._http_pool = urllib3.PoolManager()

        self.exec_ctx = ensure_deephaven_server(
            auto_start=self.config.auto_start_server,
            port=server_port,
        )
        self.ws_multiplexer = WebSocketMultiplexer(
            self.config, send_hook=ws_send_hook
        )
        self._active_builders: list[FeedBuilder] = []

        self.stocks = StocksClient(self)
        self.options = OptionsClient(self)
        self.futures = FuturesClient(self)
        self.indices = IndicesClient(self)
        self.forex = ForexClient(self)
        self.crypto = CryptoClient(self)
        self.economy = EconomyClient(self)
        self.partners = PartnersClient(self)
        self.flat_files = FlatFilesClient(self, s3_opener=s3_opener)

    def register_builder(self, builder: FeedBuilder) -> None:
        """Track an active :class:`FeedBuilder` for cleanup on :meth:`close`."""
        if builder not in self._active_builders:
            self._active_builders.append(builder)

    def _build_url(
        self, path_or_url: str, params: Mapping[str, Any] | None
    ) -> str:
        if path_or_url.startswith(("http://", "https://")):
            url = path_or_url
        else:
            clean_path = (
                path_or_url
                if path_or_url.startswith("/")
                else f"/{path_or_url}"
            )
            url = f"{self.config.rest_base_url}{clean_path}"

        if params:
            clean_params = {
                k: v for k, v in params.items() if v is not None
            }
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
        if self._http_handler is not None:
            return self._http_handler(url, params, headers)

        resp = self._http_pool.request("GET", url, headers=dict(headers))
        raw_text = resp.data.decode("utf-8") if resp.data else "{}"
        parsed = json.loads(raw_text) if raw_text.strip() else {}
        return int(resp.status), parsed

    def request_json(
        self,
        path_or_url: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        """Perform a GET request to a Massive REST endpoint with retry/backoff and entitlement checking."""
        url = self._build_url(path_or_url, params)
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Accept": "application/json",
        }

        attempt = 0
        while True:
            status, payload = self._execute_http(url, params, headers)
            if 200 <= status < 300:
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
        for builder in list(self._active_builders):
            builder.stop()
        self._active_builders.clear()
        self.ws_multiplexer.close()

    def __enter__(self) -> MassiveSession:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
