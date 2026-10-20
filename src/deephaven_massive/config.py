"""Configuration and authentication utilities for deephaven-massive."""

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
    """Raised when an API key lacks entitlement for a Massive endpoint or WebSocket feed."""


@dataclass(frozen=True)
class MassiveConfig:
    """Immutable configuration for a :class:`MassiveSession`."""

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
        """Create a validated :class:`MassiveConfig` from arguments and environment variables."""
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
        """Return True if both S3 access key ID and secret access key are configured."""
        return bool(self.s3_access_key_id and self.s3_secret_access_key)

    def resolve_ws_url(
        self,
        market: str,
        *,
        feed_override: str | None = None,
        is_fmv: bool = False,
        venue: str | None = None,
    ) -> str:
        """Resolve the exact WebSocket URL for a market, feed tier, FMV channel, and futures venue."""
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
