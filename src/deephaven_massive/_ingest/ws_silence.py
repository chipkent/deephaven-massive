"""The silence check of a WebSocket connection: it warns when no frame arrives in trading hours.

A connection that holds acknowledged subscriptions and has received no frame of any kind for
`NO_FRAMES_WARNING` seconds, during its asset class's regular trading hours
(`in_trading_hours`), logs one WARNING that its live tables may be stale and shows a note in
every stream's `Error`; the streams keep running, since quiet symbols and market holidays are
silent too. The first frame after it clears the note and logs the recovery at INFO. A
connection that is not connected is the outage's, not this check's: an outage ends the silence
episode without a log line, and its own note replaces the silence's.

`Connection` (`_ingest/ws_connection.py`) owns one `SilenceCheck`, runs `check` with each
traffic summary, `heard` on every frame, and `end` when an outage begins, all on its client's
loop thread.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.trading_hours import in_trading_hours
from deephaven_massive._tables.diagnostics import utc_text

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._enums import AssetClass

__all__ = ["NO_FRAMES_WARNING", "SilenceCheck"]

logger = logging.getLogger(__name__)

NO_FRAMES_WARNING: Final = 300.0
"""Seconds a connection with acknowledged subscriptions may go without a frame during its asset
class's regular trading hours before it logs a WARNING that its live tables may be stale. Five
minutes, since a connection carrying only a few quiet symbols can go a minute or two without a
trade; the check runs with each of the connection's traffic summaries."""


class SilenceCheck:
    """One connection's silence episodes, each logged once as a WARNING. Loop-confined.

    Attributes:
        note: The `Error` of every stream that is not down during the episode under way, or
            None outside one.
    """

    __slots__ = ("_asset_class", "_on_note", "_quiet_at", "_url", "note")

    def __init__(self, url: str, asset_class: AssetClass, *, on_note: Callable[[], None]) -> None:
        """Start with no episode.

        Args:
            url: The connection's URL, which each log line names.
            asset_class: The class whose trading hours the check keeps.
            on_note: Shows `note` on every stream, called each time it changes.
        """
        self._url = url
        self._asset_class = asset_class
        self._on_note = on_note
        self._quiet_at: int | None = None
        self.note: str | None = None

    def check(self, now: int, *, connected: bool, acknowledged: int, last_frame: int) -> None:
        """Warn once per episode when the connection goes silent while its market trades.

        Args:
            now: The current time, in nanoseconds since the epoch.
            connected: Whether the connection is connected; one that is not is the outage's.
            acknowledged: How many topics Massive has acknowledged on it.
            last_frame: When its last frame of any kind arrived or its socket opened, in
                nanoseconds since the epoch.
        """
        if self._quiet_at is not None:
            return
        silent = (now - last_frame) / 1e9
        if (
            not connected
            or not acknowledged
            or silent < NO_FRAMES_WARNING
            or not in_trading_hours(self._asset_class, datetime.fromtimestamp(now / 1e9, UTC))
        ):
            return
        self._quiet_at = now
        logger.warning(
            "%s: no frame for %.0f s during %s trading hours, though %d topics are "
            "acknowledged, so its live tables may be stale; a market holiday or symbols that "
            "are not trading explain it, and m.connections() shows the connection's state",
            self._url,
            silent,
            self._asset_class.value,
            acknowledged,
        )
        self.note = (
            f"no data since {utc_text(last_frame / 1e9)} during {self._asset_class.value} "
            "trading hours; quiet symbols or a market holiday may explain it"
        )
        self._on_note()

    def heard(self) -> None:
        """End a silence episode on a frame's arrival: log the recovery and clear the note."""
        if self._quiet_at is None:
            return
        self._quiet_at = None
        logger.info("%s: frames arrive again", self._url)
        self.note = None
        self._on_note()

    def end(self) -> None:
        """End a silence episode because an outage began, whose own note replaces it.

        Logs nothing and leaves showing the notes to the outage.
        """
        self._quiet_at = None
        self.note = None
