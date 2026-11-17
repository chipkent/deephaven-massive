"""The documentation examples the `responses` group records: what each expects.

Each example is a small piece of user code whose replies from Massive the group records
(`replies`, which holds each example's code in `replies.CALLS`, by name). The catalog lives here,
apart from that code, because the live run's parent schedules the examples by name and starts no
JVM, and the code imports the library, which imports Deephaven. This module imports neither.

Every example runs whatever the key holds: when Massive refuses one, its refusal is the reply
recorded, and the example is recorded `refused`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

DATA: Final = "data"
REPLY: Final = "reply"
"""What an example expects: data, or any reply at all."""


@dataclass(frozen=True)
class Example:
    """One documentation example: user code whose replies are recorded.

    Attributes:
        name: The file name of its replies, such as `"nvda-daily-2024-06-adjusted"`, and the key
            of its code in `replies.CALLS`.
        expects: `DATA` or `REPLY`.
        stream: Whether it is a stream, held for `replies.STREAM_SECONDS` (left out by
            `--any-time`).
    """

    name: str
    expects: str = DATA
    stream: bool = False


EXAMPLES: Final = (
    Example("nvda-daily-2024-06-adjusted"),
    Example("nvda-daily-2024-06-as-traded"),
    Example("nvda-splits"),
    Example("market-status"),
    Example("market-holidays"),
    Example("ticker-details"),
    Example("daily-bars-last-year"),
    Example("dividends-last-year"),
    Example("nvda-minute-bars-last-trading-day"),
    Example("nvda-previous-day-bar"),
    Example("treasury-yields-last-month"),
    Example("unknown-ticker-details", expects=REPLY),
    Example("stocks-snapshot"),
    Example("stocks-last-trade"),
    Example("stocks-last-quote"),
    Example("options-snapshot", expects=REPLY),
    Example("indices-snapshot"),
    Example("forex-last-quote"),
    Example("crypto-fmv-stream", stream=True),
    Example("futures-snapshot"),
    Example("stream-stock-minute-bars", stream=True),
    Example("stream-crypto-minute-bars", stream=True),
    Example("stream-forex-quotes", stream=True),
)
"""Every example, in the order they run."""
