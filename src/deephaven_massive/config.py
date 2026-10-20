"""Configuration, feed-tier routing, and entitlement exceptions for `deephaven-massive`.

This module defines `MassiveConfig`, the immutable configuration container used by
`MassiveSession` to manage REST authentication, WebSocket feed-tier host resolution,
S3 Flat File credentials, and micro-batching performance parameters.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

FEED_HOST_MAP: dict[str, str] = {
    "realtime": "socket.massive.com",
    "delayed": "delayed.massive.com",
    "business": "business.massive.com",
    "delayed_business": "delayed-business.massive.com",
    "nasdaq": "nasdaqfeed.massive.com",
    "polyfeed": "polyfeed.massive.com",
    "polyfeed_plus": "polyfeedplus.massive.com",
    "starter": "starterfeed.massive.com",
    "launchpad": "launchpad.massive.com",
}

VALID_FUTURES_VENUES: frozenset[str] = frozenset({"cme", "cbot", "nymex", "comex"})


class MassiveEntitlementError(PermissionError):
    """Raised when an API key lacks entitlement for a Massive REST endpoint or WebSocket feed.

    Massive.com enforces subscription tiers per asset class and feed latency (for
    example, real-time vs. 15-minute delayed equities, OPRA options, or Fair Market
    Value business feeds). When a REST request returns HTTP 403 or a WebSocket
    handshake returns `auth_failed` / `not_authorized`, `MassiveEntitlementError`
    is raised immediately rather than retrying.
    """


@dataclass(frozen=True)
class MassiveConfig:
    """Immutable configuration governing a `MassiveSession`.

    Prefer constructing instances via `MassiveConfig.from_args`, which resolves missing
    credentials from standard environment variables (`MASSIVE_API_KEY`, `POLYGON_API_KEY`,
    `MASSIVE_S3_ACCESS_KEY_ID`, and `MASSIVE_S3_SECRET_ACCESS_KEY`) and validates
    numeric thresholds.

    Attributes:
        api_key: Massive.com (or Polygon.io) REST and WebSocket API key.
        feed: Default WebSocket feed tier (`"realtime"`, `"delayed"`, `"business"`,
            `"delayed_business"`, `"nasdaq"`, `"polyfeed"`, `"polyfeed_plus"`,
            `"starter"`, `"launchpad"`, or a custom hostname).
        rest_base_url: Base URL for Massive REST API calls (without trailing slash).
        s3_access_key_id: Optional S3 access key ID for Massive Flat Files.
        s3_secret_access_key: Optional S3 secret access key for Massive Flat Files.
        s3_endpoint_url: S3-compatible endpoint URL for Massive Flat Files.
        s3_bucket: S3 bucket name hosting Massive Flat Files (defaults to `"flatfiles"`).
        flush_interval_ms: Milliseconds between periodic micro-batch flushes from
            WebSocket buffers into Deephaven `TablePublisher` instances.
        max_batch_size: Maximum buffered WebSocket rows before an immediate flush is triggered.
        max_retries: Maximum retry attempts with exponential backoff for transient HTTP
            errors (`429`, `500`, `502`, `503`, `504`).
        flat_file_ticker_threshold: Symbol count at or above which `source="auto"`
            historical queries prefer S3 Flat Files over REST pagination when S3
            credentials are configured.
        auto_start_server: Whether `MassiveSession` should automatically initialize an
            embedded `deephaven_server.Server` if none is running.
    """

    api_key: str
    feed: str = "realtime"
    rest_base_url: str = "https://api.massive.com"
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_endpoint_url: str = "https://files.massive.com"
    s3_bucket: str = "flatfiles"
    flush_interval_ms: int = 50
    max_batch_size: int = 10_000
    max_retries: int = 3
    flat_file_ticker_threshold: int = 50
    auto_start_server: bool = True

    @classmethod
    def from_args(
        cls,
        api_key: str | None = None,
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
    ) -> MassiveConfig:
        """Create a validated `MassiveConfig` from explicit arguments and environment variables.

        Args:
            api_key: Explicit API key. If omitted, falls back to `MASSIVE_API_KEY` and
                then `POLYGON_API_KEY` in `os.environ`.
            feed: Default WebSocket feed tier name or custom host.
            rest_base_url: Base URL for Massive REST requests.
            s3_access_key_id: Explicit S3 access key ID. Falls back to
                `MASSIVE_S3_ACCESS_KEY_ID` in `os.environ`.
            s3_secret_access_key: Explicit S3 secret access key. Falls back to
                `MASSIVE_S3_SECRET_ACCESS_KEY` in `os.environ`.
            s3_endpoint_url: S3 endpoint URL for Flat Files.
            s3_bucket: S3 bucket name for Flat Files.
            flush_interval_ms: Micro-batch flush interval in milliseconds (must be > 0).
            max_batch_size: Maximum rows per micro-batch before immediate flush (must be > 0).
            max_retries: Maximum HTTP retry count for transient errors (must be >= 0).
            flat_file_ticker_threshold: Symbol count threshold for `source="auto"` S3 routing.
            auto_start_server: Whether to start an embedded Deephaven server if needed.

        Returns:
            A validated, immutable `MassiveConfig` instance.

        Raises:
            ValueError: If no API key is provided or found in the environment, or if
                `flush_interval_ms <= 0`, `max_batch_size <= 0`, or `max_retries < 0`.
        """
        resolved_key = (
            api_key
            or os.environ.get("MASSIVE_API_KEY")
            or os.environ.get("POLYGON_API_KEY")
        )
        if not resolved_key or not resolved_key.strip():
            raise ValueError(
                "Massive API key is required. Pass api_key='...' or set MASSIVE_API_KEY / POLYGON_API_KEY."
            )

        resolved_s3_key = s3_access_key_id or os.environ.get("MASSIVE_S3_ACCESS_KEY_ID")
        resolved_s3_secret = s3_secret_access_key or os.environ.get(
            "MASSIVE_S3_SECRET_ACCESS_KEY"
        )

        if flush_interval_ms <= 0:
            raise ValueError("flush_interval_ms must be > 0")
        if max_batch_size <= 0:
            raise ValueError("max_batch_size must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")

        return cls(
            api_key=resolved_key.strip(),
            feed=feed.strip().lower(),
            rest_base_url=rest_base_url.rstrip("/"),
            s3_access_key_id=resolved_s3_key.strip() if resolved_s3_key else None,
            s3_secret_access_key=(
                resolved_s3_secret.strip() if resolved_s3_secret else None
            ),
            s3_endpoint_url=s3_endpoint_url.rstrip("/"),
            s3_bucket=s3_bucket.strip(),
            flush_interval_ms=flush_interval_ms,
            max_batch_size=max_batch_size,
            max_retries=max_retries,
            flat_file_ticker_threshold=flat_file_ticker_threshold,
            auto_start_server=auto_start_server,
        )

    @property
    def has_s3_credentials(self) -> bool:
        """Return `True` if both S3 access key ID and secret access key are configured."""
        return bool(self.s3_access_key_id and self.s3_secret_access_key)

    def resolve_ws_url(
        self,
        market: str,
        *,
        feed_override: str | None = None,
        is_fmv: bool = False,
        venue: str | None = None,
    ) -> str:
        """Resolve the WebSocket URL for a market, feed tier, FMV channel, and futures venue.

        Args:
            market: Asset class path segment (`"stocks"`, `"options"`, `"futures"`,
                `"indices"`, `"forex"`, or `"crypto"`).
            feed_override: Optional per-feed tier override (`"realtime"`, `"delayed"`,
                `"business"`, `"delayed_business"`, etc.). Defaults to `self.feed`.
            is_fmv: If `True`, routes the connection to Massive's Fair Market Value
                business WebSocket cluster (`business.massive.com` or
                `delayed-business.massive.com`).
            venue: Optional futures exchange venue (`"cme"`, `"cbot"`, `"nymex"`, or
                `"comex"`). Appended as `/{market}/{venue}` when provided.

        Returns:
            A fully qualified `wss://` WebSocket endpoint URL.

        Raises:
            ValueError: If `venue` is provided and is not one of `"cme"`, `"cbot"`,
                `"nymex"`, or `"comex"`.
        """
        effective_feed = (feed_override or self.feed).strip().lower()
        if is_fmv:
            host = (
                FEED_HOST_MAP["delayed_business"]
                if "delayed" in effective_feed
                else FEED_HOST_MAP["business"]
            )
        else:
            host = FEED_HOST_MAP.get(effective_feed, effective_feed)

        clean_market = market.strip().lower()
        path = f"/{clean_market}"
        if venue is not None:
            clean_venue = venue.strip().lower()
            if clean_venue not in VALID_FUTURES_VENUES:
                raise ValueError(
                    f"Unsupported futures venue '{venue}'. Expected one of {sorted(VALID_FUTURES_VENUES)}."
                )
            path = f"{path}/{clean_venue}"

        return f"wss://{host}{path}"
